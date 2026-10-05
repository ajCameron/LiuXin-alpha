# -*- coding: utf-8 -*-
"""
Provide icu utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise icu through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import builtins
import locale as _locale
import unicodedata
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple


UPPER_CASE = 0
LOWER_CASE = 1


def set_default_encoding(enc: str) -> None:
    # ICU sets internal encodings; Python already uses Unicode.
    """
    Set default encoding under the documented compatibility and safety rules.

    Example:
        Exercise set default encoding through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def set_filesystem_encoding(enc: str) -> None:
    """
    Set filesystem encoding under the documented compatibility and safety rules.

    Example:
        Exercise set filesystem encoding through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def change_case(s, which: int = UPPER_CASE, locale: Optional[str] = None):
    """
    Perform the change case utility operation under explicit compatibility rules.

    Example:
        Exercise change case through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :param which: Value supplied for which under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if locale is None:
        # Mirror the extension's explicit NotImplementedError for missing locale
        raise NotImplementedError("You must specify a locale")
    txt = s if isinstance(s, str) else str(s)
    if which == UPPER_CASE:
        return txt.upper()
    return txt.lower()


def swap_case(s):
    """
    Perform the swap case utility operation under explicit compatibility rules.

    Example:
        Exercise swap case through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt = s if isinstance(s, str) else str(s)
    return txt.swapcase()


def normalize(s, form: str = "NFC"):
    """
    Perform the normalize utility operation under explicit compatibility rules.

    Example:
        Exercise normalize through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :param form: Value supplied for form under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt = s if isinstance(s, str) else str(s)
    try:
        return unicodedata.normalize(form, txt)
    except Exception:
        return txt


def chr(code: int) -> str:
    """
    Perform the chr utility operation under explicit compatibility rules.

    Example:
        Exercise chr through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param code: Value supplied for code under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return builtins.chr(code)  # type: ignore[name-defined]


def character_name(ch: str) -> str:
    """
    Perform the character name utility operation under explicit compatibility rules.

    Example:
        Exercise character name through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param ch: Value supplied for ch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not ch:
        return ""
    return unicodedata.name(ch[0], "")


def character_name_from_code(code: int) -> str:
    """
    Perform the character name from code utility operation under explicit compatibility rules.

    Example:
        Exercise character name from code through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param code: Value supplied for code under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return unicodedata.name(builtins.chr(code), "")  # type: ignore[name-defined]
    except Exception:
        return ""


def string_length(s) -> int:
    """
    Perform the string length utility operation under explicit compatibility rules.

    Example:
        Exercise string length through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt = s if isinstance(s, str) else str(s)
    return len(txt)


def utf16_length(s) -> int:
    """
    Perform the utf16 length utility operation under explicit compatibility rules.

    Example:
        Exercise utf16 length through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt = s if isinstance(s, str) else str(s)
    # length in UTF-16 code units
    return len(txt.encode("utf-16-le")) // 2


def roundtrip(s, enc: str = "utf-8"):
    # Best-effort: encode+decode
    """
    Perform the roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise roundtrip through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param s: Value supplied for s under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt = s if isinstance(s, str) else str(s)
    return txt.encode(enc, "replace").decode(enc, "replace")


def get_available_transliterators() -> List[str]:
    """
    Return available transliterators under the documented compatibility and safety rules.

    Example:
        Exercise get available transliterators through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return []


def available_locales_for_break_iterator() -> List[str]:
    """
    Perform the available locales for break iterator utility operation under explicit compatibility rules.

    Example:
        Exercise available locales for break iterator through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return []


@dataclass
class Collator:
    """
    Provide the Collator utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Collator through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    locale: str

    def __post_init__(self) -> None:
        """
        Initialize and validate the Collator state.

        Example:
            Exercise Collator.  post init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: None; validated state is stored on the receiving object.
        """
        try:
            _locale.setlocale(_locale.LC_COLLATE, self.locale)
        except Exception:
            # Ignore invalid locales; continue with default collation
            pass

    def sort_key(self, s) -> bytes:
        """
        Perform the sort key utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.sort key through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param s: Value supplied for s under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        txt = s if isinstance(s, str) else str(s)
        try:
            k = _locale.strxfrm(txt)
        except Exception:
            k = txt
        return k.encode("utf-8", "surrogatepass")

    def strcmp(self, a, b) -> int:
        """
        Perform the strcmp utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.strcmp through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param a: Value supplied for a under the utility contract.
        :param b: Value supplied for b under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ka = self.sort_key(a)
        kb = self.sort_key(b)
        return -1 if ka < kb else 1 if ka > kb else 0

    def find(self, haystack, needle, start: int = 0) -> int:
        """
        Perform the find utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.find through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param haystack: Value supplied for haystack under the utility contract.
        :param needle: Value supplied for needle under the utility contract.
        :param start: Value supplied for start under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        h = haystack if isinstance(haystack, str) else str(haystack)
        n = needle if isinstance(needle, str) else str(needle)
        return h.find(n, start)

    def contains(self, haystack, needle) -> bool:
        """
        Perform the contains utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.contains through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param haystack: Value supplied for haystack under the utility contract.
        :param needle: Value supplied for needle under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.find(haystack, needle) != -1

    def contractions(self):
        # True ICU returns contraction expansions; we don't implement it.
        """
        Perform the contractions utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.contractions through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ()

    def clone(self):
        """
        Perform the clone utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.clone through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Collator(self.locale)

    def startswith(self, s, prefix) -> bool:
        """
        Perform the startswith utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.startswith through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param s: Value supplied for s under the utility contract.
        :param prefix: Text prepended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        txt = s if isinstance(s, str) else str(s)
        p = prefix if isinstance(prefix, str) else str(prefix)
        return txt.startswith(p)

    def collation_order(self, s) -> List[int]:
        # Compatibility: return a list of integer "orders" from the sort key bytes.
        """
        Perform the collation order utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.collation order through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param s: Value supplied for s under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(self.sort_key(s))


# Break iterator type constants are not mirrored here; users can pass any int.
class BreakIterator:
    """
    Provide the BreakIterator utility contract with explicit state and cleanup behavior.

    Example:
        Exercise BreakIterator through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    def __init__(self, break_iterator_type: int, locale: str):
        """
        Initialize and validate the BreakIterator state.

        Example:
            Exercise BreakIterator.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param break_iterator_type: Value supplied for break iterator type under the utility
            contract.
        :param locale: Value supplied for locale under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.type = int(break_iterator_type)
        self.locale = locale
        self._text = ""

    def set_text(self, s) -> None:
        """
        Set text under the documented compatibility and safety rules.

        Example:
            Exercise BreakIterator.set text through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param s: Value supplied for s under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._text = s if isinstance(s, str) else str(s)

    def split2(self) -> List[Tuple[int, int]]:
        # Approximation: return spans of "words" (letters/digits/underscore), allowing hyphens inside.
        """
        Perform the split2 utility operation under explicit compatibility rules.

        Example:
            Exercise BreakIterator.split2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        txt = self._text
        spans: List[Tuple[int, int]] = []
        for m in re.finditer(r"[A-Za-z0-9_]+(?:-[A-Za-z0-9_]+)*", txt):
            spans.append((m.start(), m.end() - m.start()))
        return spans

    def index(self, token) -> int:
        """
        Perform the index utility operation under explicit compatibility rules.

        Example:
            Exercise BreakIterator.index through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param token: Value supplied for token under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        t = token if isinstance(token, str) else str(token)
        if not t:
            return -1
        return self._text.find(t)
