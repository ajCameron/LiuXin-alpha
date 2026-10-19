#!/usr/bin/env python
# vim:fileencoding=utf-8


"""
Provide icu utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise icu through a consuming regression::

        python -m pytest -q tests/utils/text/test_text_core.py
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import sys
import codecs
import logging
import threading
from contextvars import ContextVar

from LiuXin_alpha.configuration import current_preferences

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

logger = logging.getLogger(__name__)


def icu_lower(target_str: str) -> str:
    """
    Transform a string to the lower case form.

    Example:
        Exercise icu lower through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param target_str: Value supplied for target str under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return target_str.lower()


lower = icu_lower

is_narrow_build = sys.maxunicode < 0x10FFFF

icu_loaded = False
try:
    import LiuXin_alpha.utils.compiled_extensions.linux.icu as _icu

    logger.debug("ICU extension module loaded")
    icu_loaded = True
except ImportError:
    wrn_str = "Unable to import _icu.\n"
    wrn_str += "falling back.\n"
    wrn_str += "try running setup.py"
    import LiuXin_alpha.utils.text.icu_fallback as _icu

    logger.info("%s", wrn_str)

from LiuXin_alpha.utils.config.config_base import tweaks

# Selection is lazy. Per-thread collators are keyed by effective settings, so
# importing this module neither reads settings nor fixes a process-wide locale.
_collator_cache = threading.local()
_locale_override = ContextVar("liuxin_icu_locale", default=None)


def _selected_locale():
    from LiuXin_alpha.utils.localization import get_lang

    store = current_preferences()
    override = _locale_override.get()
    if override is not None and override[0] is store and override[1] is not None:
        return override[1]
    return tweaks.get("locale_for_sorting") or get_lang()


def _configured_collator(kind):
    locale = _selected_locale()
    numeric = bool(tweaks.get("numeric_collation", True))
    key = (locale, kind, numeric)
    cache = getattr(_collator_cache, "values", None)
    if cache is None:
        cache = _collator_cache.values = {}
    if key not in cache:
        try:
            result = _icu.Collator(locale)
        except Exception as error:
            logger.warning("Failed to load collator for locale %r (%r); using English", locale, error)
            result = _icu.Collator("en")
        if kind == "primary":
            result.strength = _icu.UCOL_PRIMARY
        elif kind in {"sort", "numeric"}:
            result.strength = _icu.UCOL_SECONDARY
        if kind in {"sort", "case_sensitive"}:
            result.numeric = numeric
        elif kind == "numeric":
            result.numeric = True
        if kind == "case_sensitive":
            result.upper_first = True
        cache[key] = result
    return cache[key]


_none = ""
_none2 = b""
_cmap = {}

icu_unicode_version = getattr(_icu, "unicode_version", None)
_nmodes = {m: getattr(_icu, "UNORM_" + m, None) for m in ("NFC", "NFD", "NFKC", "NFKD", "NONE", "DEFAULT", "FCD")}

# Ensure that the python internal filesystem and default encodings are not ASCII
def is_ascii(name):
    """
    Return or update whether is ascii holds for the compatibility value.

    Example:
        Exercise is ascii through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: True when the documented condition holds; otherwise False.
    """
    try:
        return codecs.lookup(name).name == b"ascii"
    except (TypeError, LookupError):
        return True


try:
    if is_ascii(sys.getdefaultencoding()):
        _icu.set_default_encoding(b"utf-8")
except Exception:
    logger.exception("Failed to set ICU default encoding to UTF-8")

try:
    if is_ascii(sys.getfilesystemencoding()):
        _icu.set_filesystem_encoding(b"utf-8")
except Exception:
    logger.exception("Failed to set ICU filesystem encoding to UTF-8")
del is_ascii


def collator():
    """
    Perform the collator utility operation under explicit compatibility rules.

    Example:
        Exercise collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _configured_collator('base')


def change_locale(locale=None):
    """
    Override the locale for this context and preference store without persisting it.

    Example:
        Exercise change locale through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param locale: Value supplied for locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _locale_override.set((current_preferences(), locale))
    _collator_cache.values = {}


def primary_collator():
    """
    Ignores case differences and accented characters

    Example:
        Exercise primary collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _configured_collator('primary')


def sort_collator():
    """
    Ignores case differences and recognizes numbers in strings (if the tweak is set)

    Example:
        Exercise sort collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _configured_collator('sort')


def numeric_collator():
    """
    Uses natural sorting for numbers inside strings so something2 will sort before something10

    Example:
        Exercise numeric collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _configured_collator('numeric')


def case_sensitive_collator():
    """
    Always sorts upper case letter before lower case

    Example:
        Exercise case sensitive collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _configured_collator('case_sensitive')


# Templates that will be used to generate various concrete
# function implementations based on different collators, to allow lazy loading
# of collators, with maximum runtime performance


_sort_key_template = """
def {name}(obj):
    try:
        try:
            return {collator_func}().{func}(obj)
        except AttributeError:
            return {collator_func}().{func}(obj)
    except TypeError:
        if isinstance(obj, bytes):
            try:
                obj = obj.decode(sys.getdefaultencoding())
            except ValueError:
                return obj
            return {collator_func}().{func}(obj)
    return b''
"""

_strcmp_template = """
def {name}(a, b):
    try:
        try:
            return {collator_func}().{func}(a, b)
        except AttributeError:
            return {collator_func}().{func}(a, b)
    except TypeError:
        if isinstance(a, bytes):
            try:
                a = a.decode(sys.getdefaultencoding())
            except ValueError:
                return cmp(a, b)
        elif a is None:
            a = u''
        if isinstance(b, bytes):
            try:
                b = b.decode(sys.getdefaultencoding())
            except ValueError:
                return cmp(a, b)
        elif b is None:
            b = u''
        return {collator_func}().{func}(a, b)
"""

_change_case_template = """
def {name}(x):
    try:
        try:
            return _icu.change_case(x, _icu.{which}, _selected_locale())
        except NotImplementedError:
            collator()  # initializes the selected locale
            return _icu.change_case(x, _icu.{which}, _selected_locale())
    except TypeError:
        if isinstance(x, bytes):
            try:
                x = x.decode(sys.getdefaultencoding())
            except ValueError:
                return x
            return _icu.change_case(x, _icu.{which}, _selected_locale())
        raise
"""


def _make_func(template, name, **kwargs):
    """
    Perform the make func utility operation under explicit compatibility rules.

    Example:
        Exercise  make func through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param template: Template expression parsed or evaluated.
    :param name: Field, file, function or resource name addressed by the operation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    local_globals = globals()
    kwargs["name"] = name
    kwargs["func"] = kwargs.get("func", "sort_key")
    exec(template.format(**kwargs), local_globals)
    return local_globals[name]


# }}}

# ################ The string functions ########################################
sort_key = _make_func(
    _sort_key_template,
    "sort_key",
    collator="_sort_collator",
    collator_func="sort_collator",
)

numeric_sort_key = _make_func(
    _sort_key_template,
    "numeric_sort_key",
    collator="_numeric_collator",
    collator_func="numeric_collator",
)

primary_sort_key = _make_func(
    _sort_key_template,
    "primary_sort_key",
    collator="_primary_collator",
    collator_func="primary_collator",
)

case_sensitive_sort_key = _make_func(
    _sort_key_template,
    "case_sensitive_sort_key",
    collator="_case_sensitive_collator",
    collator_func="case_sensitive_collator",
)

collation_order = _make_func(
    _sort_key_template,
    "collation_order",
    collator="_sort_collator",
    collator_func="sort_collator",
    func="collation_order",
)

strcmp = _make_func(
    _strcmp_template,
    "strcmp",
    collator="_sort_collator",
    collator_func="sort_collator",
    func="strcmp",
)

case_sensitive_strcmp = _make_func(
    _strcmp_template,
    "case_sensitive_strcmp",
    collator="_case_sensitive_collator",
    collator_func="case_sensitive_collator",
    func="strcmp",
)

primary_strcmp = _make_func(
    _strcmp_template,
    "primary_strcmp",
    collator="_primary_collator",
    collator_func="primary_collator",
    func="strcmp",
)

upper = _make_func(_change_case_template, "upper", which="UPPER_CASE")

lower = _make_func(_change_case_template, "lower", which="LOWER_CASE")

title_case = _make_func(_change_case_template, "title_case", which="TITLE_CASE")


def capitalize(x):
    """
    Perform the capitalize utility operation under explicit compatibility rules.

    Example:
        Exercise capitalize through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return upper(x[0]) + lower(x[1:])
    except (IndexError, TypeError, AttributeError):
        return x


try:
    swapcase = _icu.swap_case
except AttributeError:  # For people running from source
    swapcase = lambda x: x.swapcase()

find = _make_func(
    _strcmp_template,
    "find",
    collator="_collator",
    collator_func="collator",
    func="find",
)

primary_find = _make_func(
    _strcmp_template,
    "primary_find",
    collator="_primary_collator",
    collator_func="primary_collator",
    func="find",
)

contains = _make_func(
    _strcmp_template,
    "contains",
    collator="_collator",
    collator_func="collator",
    func="contains",
)

primary_contains = _make_func(
    _strcmp_template,
    "primary_contains",
    collator="_primary_collator",
    collator_func="primary_collator",
    func="contains",
)

startswith = _make_func(
    _strcmp_template,
    "startswith",
    collator="_collator",
    collator_func="collator",
    func="startswith",
)

primary_startswith = _make_func(
    _strcmp_template,
    "primary_startswith",
    collator="_primary_collator",
    collator_func="primary_collator",
    func="startswith",
)

safe_chr = _icu.chr


def character_name(string):
    """
    Perform the character name utility operation under explicit compatibility rules.

    Example:
        Exercise character name through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param string: Value supplied for string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return _icu.character_name(six_unicode(string)) or None
    except (TypeError, ValueError, KeyError):
        pass


def character_name_from_code(code):
    """
    Perform the character name from code utility operation under explicit compatibility rules.

    Example:
        Exercise character name from code through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param code: Value supplied for code under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return _icu.character_name_from_code(code) or ""
    except (TypeError, ValueError, KeyError):
        return ""


def normalize(text, mode="NFC"):
    # This is very slightly slower than using unicodedata.normalize, so stick with
    # that unless you have very good reasons not too. Also, it's speed
    # decreases on wide python builds, where conversion to/from ICU's string
    # representation is slower.
    """
    Perform the normalize utility operation under explicit compatibility rules.

    Example:
        Exercise normalize through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param text: Text parsed, normalized or rendered.
    :param mode: Open or adapter mode controlling read/write behavior.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _icu.normalize(_nmodes[mode], six_unicode(text))


def contractions(col=None):
    """
    Perform the contractions utility operation under explicit compatibility rules.

    Example:
        Exercise contractions through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param col: Value supplied for col under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _cmap
    col = col if col is not None else collator()
    ans = _cmap.get(col, None)
    if ans is None:
        ans = col.contractions()
        ans = frozenset(filter(None, ans))
        _cmap[col] = ans
    return ans


def partition_by_first_letter(items, reverse=False, key=lambda x: x):
    """
    Build a list of 'equal' first letters by noticing changes in ICU's 'ordinal' for the first letter.

    Example:
        Exercise partition by first letter through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param items: Value supplied for items under the utility contract.
    :param reverse: Value supplied for reverse under the utility contract.
    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from collections import OrderedDict

    items = sorted(items, key=lambda x: sort_key(key(x)), reverse=reverse)
    ans = OrderedDict()
    last_c, last_ordnum = " ", 0
    for item in items:
        c = upper(key(item) or " ")
        ordnum, ordlen = collation_order(c)
        if last_ordnum != ordnum:
            if not is_narrow_build:
                ordlen = 1
            last_c = c[0:ordlen]
            last_ordnum = ordnum
        try:
            ans[last_c].append(item)
        except KeyError:
            ans[last_c] = [item]
    return ans


# Return the number of unicode codepoints in a string
string_length = _icu.string_length if is_narrow_build else len

# Return the number of UTF-16 codepoints in a string
# https://pypi.org/project/PyICU/
utf16_length = len if is_narrow_build else _icu.utf16_length


def safe_lower(x):
    """
    Perform the safe lower utility operation under explicit compatibility rules.

    Example:
        Exercise safe lower through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return lower(x)
    except (TypeError, ValueError, KeyError, AttributeError):
        return x


################################################################################


if __name__ == "__main__":
    from LiuXin_alpha.utils.calibre.utils.icu_test import run

    run(verbosity=4)
