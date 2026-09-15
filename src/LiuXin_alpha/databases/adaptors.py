#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Adapt legacy field values for display, cache updates and database persistence.

General field adapters and custom-column adapters retain different null, validation and normalization rules. Field factories select a datatype converter before applying name-specific wrappers; annotations do not validate runtime inputs. Existing byte-handler and custom-boolean datetime-name limitations are documented at the affected helpers.
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
    Format datetime instances as UTC text and preserve other inputs.

    The formatter treats a naive datetime as local time and converts to UTC. Non-datetime values, including None and strings, pass through despite the narrow annotations.

    Example:
        >>> sqlite_datetime("2024-01-01 00:00:00")
        '2024-01-01 00:00:00'
        >>> sqlite_datetime(None) is None
        True


    :param x: Value to prepare; datetime instances use the shared ISO formatter.
    :return: ISO text with a space separator for datetimes, otherwise the original object.
    """
    return isoformat(x, sep=" ") if isinstance(x, datetime) else x


def single_text(x: AnyStr) -> Optional[str]:
    """
    Decode or stringify one value, strip its edges and collapse empty text to None.

    Non-text inputs first try decode(preferred_encoding, "replace"); AttributeError falls back to six_unicode. Internal whitespace is preserved. Other decoding/stringification errors propagate.

    Example:
        >>> single_text("  Example  ")
        'Example'
        >>> single_text("   ") is None
        True


    :param x: Text, bytes, another printable value, or None.
    :return: Stripped text, or None for None/empty/whitespace-only input.
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
    Extract a trailing bracketed numeric series index when its pattern matches.

    Match stripped input against a suffix containing digits and dots, preceded by whitespace. Negative indexes do not match. A malformed float is logged and leaves the original value unchanged, including its surrounding whitespace.

    Example:
        >>> get_series_values("  Cycle [2.5]  ")
        ('Cycle', 2.5)
        >>> get_series_values(" Cycle ")
        (' Cycle ', None)


    :param val: Series text, or a false value passed through without parsing.
    :return: Pair (stripped series name, float index) on success, otherwise (original value, None).
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
    Split or iterate text values, normalize spacing and replace embedded display separators.

    False input returns an empty tuple before separator access. Byte elements in an iterable decode with replacement. A top-level byte string instead uses the misspelled error handler "replce", which can raise LookupError when decoding needs that handler. Replace the stripped ui_sep with a comma when it is ";", otherwise with a semicolon, then collapse whitespace within each token. Empty separators are not specially validated.

    Example:
        >>> multiple_text(",", ", ", "  alpha  , beta ,, ")
        ('alpha', 'beta')


    :param sep: Separator used only when splitting a single text input.
    :param ui_sep: Display separator, stripped before replacement within each token.
    :param x: Text to split, an iterable of text/byte values, or a false value.
    :return: Tuple of nonempty normalized strings in input order; duplicates remain.
    :raises LookupError: Decoding a top-level byte string needs the unregistered "replce" error handler.
    :raises ValueError: A text input is split with an empty separator.
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
    Parse text as local time and normalize undefined or non-date values.

    Strings use parse_date with assume_utc=False and as_utc=False. If undefined-date inspection raises AttributeError, replace the value with UNDEFINED_DATE. Truthy values identified as undefined also become that sentinel; None remains None. Parsing errors are not suppressed.

    Example:
        >>> adapt_datetime(None) is None
        True
        >>> adapt_datetime(123) == UNDEFINED_DATE
        True


    :param x: Date/time text or bytes, a date-like value, or None.
    :return: Parsed local-time datetime, an accepted original value, UNDEFINED_DATE, or None.
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
    Parse a date-only value with the shared timezone-safe date convention.

    Text uses parse_only_date with its UTC defaults and day-boundary adjustment, so the represented day can move near month boundaries. Other values are checked without the AttributeError fallback used by adapt_datetime.

    Example:
        >>> adapt_date(None) == UNDEFINED_DATE
        True


    :param x: Date-only text/bytes, a date-like value, or None.
    :return: Parsed or original date-like value, with None/undefined values replaced by UNDEFINED_DATE.
    :raises AttributeError: A non-null, non-string value lacks attributes required by undefined-date inspection.
    """
    if isinstance(x, (unicode, bytes)):
        x = parse_only_date(x)
    if x is None or is_date_undefined(x):
        x = UNDEFINED_DATE
    return x


def adapt_number(typ: Union[Literal[int], Literal[float]], x: AnyStr) -> Optional[Union[int, float]]:
    """
    Convert a non-null value with the requested numeric callable.

    No general whitespace stripping or boolean rejection is added. Nonempty bytes are not decoded before comparison with "none", so b"none" reaches the numeric converter and fails. Conversion exceptions propagate.

    Example:
        >>> adapt_number(int, "42")
        42
        >>> adapt_number(float, "NONE") is None
        True


    :param typ: Conversion callable, normally int or float.
    :param x: Value to convert; None, empty text/bytes and case-insensitive text "none" mean unset.
    :return: None for recognized unset values, otherwise typ(x).
    :raises ValueError: The requested numeric converter cannot parse the value.
    :raises TypeError: The callable cannot convert the supplied type.
    """

    if x is None:
        return None
    if isinstance(x, (unicode, bytes)):
        if not x or x.lower() == "none":
            return None
    return typ(x)


def adapt_bool(x: AnyStr) -> Optional[bool]:
    """
    Convert legacy boolean text or numeric values to True, False or None.

    Lowercase string inputs first. Text "true" and "false" are recognized directly; other string/byte inputs go through int then bool. Byte words are not decoded and do not match the text sentinels. Non-string values use bool, so numeric zero is False.

    Example:
        >>> adapt_bool("FALSE")
        False
        >>> adapt_bool("none") is None
        True


    :param x: Text/bytes or a value with normal Python truth semantics.
    :return: None for None or text "none"/empty text; otherwise a boolean.
    :raises ValueError: A string/byte value is neither a recognized text word nor integer-convertible.
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
    Canonicalize converted language values and retain distinct accepted results.

    Apply canonicalize_lang without code flags, so successful lookups return language names. Drop false results and results literally equal to und, zxx, mis or mul. Those code-string exclusions generally do not exclude resolved names: und becomes Undetermined and is retained. Duplicate returned names appear once; converter and canonicalizer errors propagate.

    Example:
        >>> adapt_languages(lambda value: value, ("eng", "eng", "und"))
        ('English', 'Undetermined')


    :param to_tuple: Callable producing an iterable of language values from x.
    :param x: Input passed unchanged to to_tuple.
    :return: Tuple of canonical language names in first-occurrence order.
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
    Normalize one identifier scheme and value for legacy text storage.

    Lowercase the scheme using ICU, trim it and remove colons/commas. Trim the value and replace commas with pipes; preserve value case and colons. This performs no identifier validity check or general byte decoding.

    Example:
        >>> clean_identifier(" ISBN: ", " a,b ")
        ('isbn', 'a|b')


    :param typ: Text scheme, with false values treated as empty.
    :param val: Text value, with false values treated as empty.
    :return: Pair of cleaned scheme and value; either can be empty.
    """
    typ = icu_lower(typ or "").strip().replace(":", "").replace(",", "")
    val = (val or "").strip().replace(",", "|")
    return typ, val


def adapt_identifiers(to_tuple: Callable[[str, ], dict[str, str]], x: Union[dict[str, str], str]) -> dict[str, str]:
    """
    Build a cleaned identifier dictionary from a mapping or converted colon-pairs.

    Non-dict input is partitioned at the first colon per item and collapsed into a dictionary before cleaning. Later entries win for duplicate raw or normalized schemes. Dict input bypasses to_tuple but is still copied/cleaned; no repository validation is performed.

    Example:
        >>> adapt_identifiers(None, {" ISBN: ": " a,b ", "empty": ""})
        {'isbn': 'a|b'}


    :param to_tuple: Callable producing iterable scheme:value strings when x is not a dict.
    :param x: Dictionary of identifier schemes/values, or input passed to to_tuple.
    :return: A new dictionary containing only entries with nonempty cleaned scheme and value.
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
    Select a legacy field converter from metadata and field-name overrides.

    Select text/series/comments/enumeration normalization, datetime parsing (date-only for pubdate), int/float conversion, tri-state bool, clamped rating, or composite identity by datatype. Then wrap title with a translated Unknown fallback, author_sort with empty text, authors with pipe-to-comma tuple conversion and Unknown fallback, timestamps with UNDEFINED_DATE, and series_index with 1.0 when unset. The series-index wrapper calls the base converter twice for non-null results. Languages and identifiers get additional canonicalization/cleaning. Name/datatype combinations are not validated for compatible result shapes; unknown datatypes are logged and rejected before wrappers are selected.

    Example:
        >>> get_adapter("series_index", {"datatype": "float"})(None)
        1.0


    :param name: Field name used for date selection and final special-case wrappers.
    :param metadata: Metadata requiring datatype; text also requires is_multiple, whose truthy mapping provides ui_to_list/list_to_ui.
    :return: Callable accepting one field value; conversion exceptions occur when it is invoked.
    :raises KeyError: Required datatype or text separator metadata is absent.
    :raises NotImplementedError: The datatype is not recognized.
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
            """
            Convert and clamp one rating value for the surrounding factory.

            The string "0" converts to 0 rather than None. Empty text and text "none" convert to None inside adapt_number and then fail numeric comparison; unhashable input fails the initial membership test.

            Example:
                >>> get_adapter("rating", {"datatype": "rating"})(15)
                10


            :param x: Hashable value tested against None and numeric zero before integer conversion.
            :return: None for None/numeric zero, otherwise an integer clamped to 0–10.
            :raises TypeError: The input is unhashable or conversion returns None before clamping.
            :raises ValueError: Integer conversion fails.
            """

            return None if x in {None, 0} else min(10, max(0, adapt_number(int, x)))

    elif dt == "enumeration":
        ans = single_text

    elif dt == "composite":

        def ans(x):
            """
            Return a composite field value unchanged.

            Example:
                >>> value = []
                >>> get_adapter("template", {"datatype": "composite"})(value) is value
                True


            :param x: Any object passed to the composite converter.
            :return: The identical input object; no copying or validation occurs.
            """

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
    Select a field converter using explicit datatype and separator arguments.

    Select text/series/comments/enumeration normalization, datetime parsing (date-only for pubdate), int/float conversion, tri-state bool, clamped rating, or composite identity by datatype. Then wrap title with a translated Unknown fallback, author_sort with empty text, authors with pipe-to-comma tuple conversion and Unknown fallback, timestamps with UNDEFINED_DATE, and series_index with 1.0 when unset. The series-index wrapper calls the base converter twice for non-null results. Languages and identifiers get additional canonicalization/cleaning. Name/datatype combinations are not validated for compatible result shapes; unknown datatypes are logged and rejected before wrappers are selected. Unlike get_adapter, this form takes a boolean multiple flag and separate separators; their defaults are not replaced with metadata values.

    Example:
        >>> get_adapter_from_name_and_dt("count", "int")("42")
        42


    :param name: Field name controlling special-case wrappers.
    :param datatype: Datatype selector accepted by get_adapter.
    :param is_multiple: Whether text values use the multiple-text converter.
    :param ui_to_list: Input delimiter for multiple text; None is forwarded to str.split.
    :param list_to_ui: Display separator for multiple text; a string is needed for nonempty conversion.
    :return: Callable accepting one field value.
    :raises NotImplementedError: The datatype is not recognized.
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
            """
            Convert and clamp one rating value for the surrounding factory.

            The string "0" converts to 0 rather than None. Empty text and text "none" convert to None inside adapt_number and then fail numeric comparison; unhashable input fails the initial membership test.

            Example:
                >>> get_adapter_from_name_and_dt("rating", "rating")(15)
                10


            :param x: Hashable value tested against None and numeric zero before integer conversion.
            :return: None for None/numeric zero, otherwise an integer clamped to 0–10.
            :raises TypeError: The input is unhashable or conversion returns None before clamping.
            :raises ValueError: Integer conversion fails.
            """

            return None if x in {None, 0} else min(10, max(0, adapt_number(int, x)))

    elif dt == "enumeration":
        ans = single_text

    elif dt == "composite":

        def ans(x):
            """
            Return a composite field value unchanged.

            Example:
                >>> value = []
                >>> get_adapter_from_name_and_dt("template", "composite")(value) is value
                True


            :param x: Any object passed to the composite converter.
            :return: The identical input object; no copying or validation occurs.
            """

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
    Prepare single or multiple custom-column text without the general text adapter rules.

    Multiple None becomes an empty list. Text/bytes are split before the guarded token-strip step; errors while stripping/filtering tokens are logged and wrapped as InvalidUpdate. Surviving byte tokens decode with replacement, then internal whitespace collapses. Splitting and decoding failures can propagate separately. Single columns preserve whitespace and reject non-text/non-null values.

    Example:
        >>> cc_adapt_text("  Example  ", {"is_multiple": False})
        '  Example  '
        >>> cc_adapt_text(None, {"is_multiple": True})
        []


    :param x: Text/bytes, an iterable of tokens for multiple columns, or None.
    :param d: Column metadata with is_multiple and, when splitting, multiple_seps.ui_to_list.
    :return: A list of normalized strings for multiple columns, otherwise original text/None or decoded bytes.
    :raises InvalidUpdate: A single-column value has an unsupported type or multiple-token stripping fails.
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
    Parse custom-column date text and reject booleans or numeric timestamps.

    String parsing uses assume_utc=False and as_utc=False; any exception from parsing is converted to InvalidUpdate. Exact booleans and int/float values are rejected. No datetime validation or undefined-date normalization is applied to remaining objects.

    Example:
        >>> cc_adapt_datetime(None, {}) is None
        True


    :param x: Date/time text/bytes, a date object, None or another value.
    :param d: Column metadata used only in error diagnostics.
    :return: Parsed local-time datetime for strings; other accepted values pass through unchanged.
    :raises InvalidUpdate: String parsing fails, or x is a boolean, integer or float.
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
    Parse custom boolean text while preserving the existing non-string failure.

    Text true/1, false/0 and none are recognized after lowercasing. Other string values use int conversion with failures wrapped as InvalidUpdate, and floats are rejected. For every non-string, non-float value, the existing datetime.datetime check raises AttributeError because datetime names the imported class. Thus None, booleans, integers and datetime objects currently fail at that check.

    Example:
        >>> cc_adapt_bool("true", {})
        True
        >>> cc_adapt_bool("none", {}) is None
        True


    :param x: Boolean text/bytes or another attempted value.
    :param d: Column metadata included in failure diagnostics.
    :return: True, False or None for recognized text; numeric text/bytes use bool(int(x)).
    :raises InvalidUpdate: A string cannot be converted or x is a float.
    :raises AttributeError: A non-string, non-float input reaches the shadowed datetime.datetime reference.
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
    Apply custom-column text conversion and replace false results with None.

    No membership check against configured enumeration choices is performed. Single text values keep surrounding whitespace; multiple-column normalization follows cc_adapt_text.

    Example:
        >>> cc_adapt_enum(" Active ", {"is_multiple": False})
        ' Active '
        >>> cc_adapt_enum("", {"is_multiple": False}) is None
        True


    :param x: Value passed to cc_adapt_text.
    :param d: Column metadata passed to cc_adapt_text.
    :return: Converted text/list, or None for a false converted result.
    """
    v = cc_adapt_text(x, d)
    if not v:
        v = None
    return v


def cc_adapt_number(x: Any, d) -> Optional[Union[int, float]]:
    """
    Convert a custom-column number, rejecting booleans and wrapping conversion failures.

    Booleans are rejected before conversion. Byte words are not decoded for the text sentinel check. Numeric conversion exceptions become InvalidUpdate; a missing datatype key remains a KeyError.

    Example:
        >>> cc_adapt_number("42", {"datatype": "int"})
        42
        >>> cc_adapt_number(None, {}) is None
        True


    :param x: Value to convert; None or case-insensitive text "none" is unset.
    :param d: Column metadata; datatype="int" selects int, every other datatype selects float.
    :return: None for recognized unset input, otherwise the converted integer or float.
    :raises InvalidUpdate: Input is boolean or the selected numeric conversion fails.
    :raises KeyError: A value needing conversion has no datatype metadata.
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
    Convert a custom-column rating to float and clamp it to 0–10.

    Example:
        >>> cc_adapt_rating("12", {})
        10.0
        >>> cc_adapt_rating(None, {}) is None
        True


    :param x: Numeric value or numeric text; None is unset and booleans are rejected.
    :param d: Column metadata included only in failure diagnostics.
    :return: None for None, otherwise a float clamped between 0.0 and 10.0.
    :raises InvalidUpdate: Input is boolean or float conversion raises ValueError/TypeError.
    """
    if x is None:
        return None
    if x is True or x is False:
        raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))
    try:
        return min(10.0, max(0.0, float(x)))
    except (ValueError, TypeError):
        raise InvalidUpdate("Unexpected update type - x: {} - d: {}".format(x, d))
