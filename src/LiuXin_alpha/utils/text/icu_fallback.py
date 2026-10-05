"""
Provide pure-Python text collation and boundary behavior when ICU is unavailable.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise icu fallback through a consuming regression::

        python -m pytest -q tests/utils/text/test_text_core.py
"""

from __future__ import annotations

import builtins
import re
import unicodedata
from typing import Any, Literal, cast


UPPER_CASE = 0
LOWER_CASE = 1
TITLE_CASE = 2

UCOL_PRIMARY = 0
UCOL_SECONDARY = 1

UNORM_NFC = "NFC"
UNORM_NFD = "NFD"
UNORM_NFKC = "NFKC"
UNORM_NFKD = "NFKD"
UNORM_NONE = "NFC"
UNORM_DEFAULT = "NFC"
UNORM_FCD = "NFC"

unicode_version = unicodedata.unidata_version


def _text(value: Any) -> str:
    """
    Perform the text utility operation under explicit compatibility rules.

    Example:
        Exercise  text through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else str(value)


def _without_accents(value: str) -> str:
    """
    Perform the without accents utility operation under explicit compatibility rules.

    Example:
        Exercise  without accents through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "".join(
        character
        for character in unicodedata.normalize("NFD", value)
        if unicodedata.category(character) != "Mn"
    )


def set_default_encoding(_encoding: str | bytes) -> None:
    """
    Retain the extension API; Python strings need no global encoding.

    Example:
        Exercise set default encoding through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param _encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def set_filesystem_encoding(_encoding: str | bytes) -> None:
    """
    Retain the extension API; Python owns the filesystem encoding.

    Example:
        Exercise set filesystem encoding through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param _encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def change_case(value: Any, operation: int, _locale: str | None = None) -> str:
    """
    Perform the change case utility operation under explicit compatibility rules.

    Example:
        Exercise change case through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :param operation: Value supplied for operation under the utility contract.
    :param _locale: Value supplied for locale under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = _text(value)
    if operation == UPPER_CASE:
        return text.upper()
    if operation == TITLE_CASE:
        return text.title()
    return text.lower()


def swap_case(value: Any) -> str:
    """
    Perform the swap case utility operation under explicit compatibility rules.

    Example:
        Exercise swap case through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _text(value).swapcase()


def normalize(mode: str | None, text: Any) -> str:
    """
    Perform the normalize utility operation under explicit compatibility rules.

    Example:
        Exercise normalize through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param mode: Open or adapter mode controlling read/write behavior.
    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    form = cast(
        Literal["NFC", "NFD", "NFKC", "NFKD"],
        mode if mode in {"NFC", "NFD", "NFKC", "NFKD"} else "NFC",
    )
    return unicodedata.normalize(form, _text(text))


def chr(character: int) -> str:
    """
    Perform the chr utility operation under explicit compatibility rules.

    Example:
        Exercise chr through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param character: Value supplied for character under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return builtins.chr(int(character))


def character_name(character: Any) -> str:
    """
    Perform the character name utility operation under explicit compatibility rules.

    Example:
        Exercise character name through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param character: Value supplied for character under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = _text(character)
    return unicodedata.name(text[0], "") if text else ""


def character_name_from_code(character_code: int) -> str:
    """
    Perform the character name from code utility operation under explicit compatibility rules.

    Example:
        Exercise character name from code through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param character_code: Value supplied for character code under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return unicodedata.name(builtins.chr(int(character_code)), "")
    except (TypeError, ValueError):
        return ""


def string_length(value: Any) -> int:
    """
    Perform the string length utility operation under explicit compatibility rules.

    Example:
        Exercise string length through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return len(_text(value))


def utf16_length(value: Any) -> int:
    """
    Perform the utf16 length utility operation under explicit compatibility rules.

    Example:
        Exercise utf16 length through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return len(_text(value).encode("utf-16-le")) // 2


class Collator:
    """
    Small collation adapter with the compiled extension's call conventions.

    Example:
        Exercise Collator through a consuming regression::

            python -m pytest -q tests/utils/text/test_text_core.py
    """

    def __init__(self, locale: str) -> None:
        """
        Initialize and validate the Collator state.

        Example:
            Exercise Collator.  init   through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param locale: Value supplied for locale under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.locale = locale
        self.strength = UCOL_SECONDARY
        self.numeric = False
        self.upper_first = False

    def clone(self) -> "Collator":
        """
        Perform the clone utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.clone through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clone = type(self)(self.locale)
        clone.strength = self.strength
        clone.numeric = self.numeric
        clone.upper_first = self.upper_first
        return clone

    def _comparison_text(self, value: Any) -> str:
        """
        Perform the comparison text utility operation under explicit compatibility rules.

        Example:
            Exercise Collator. comparison text through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = _text(value)
        if self.strength in {UCOL_PRIMARY, UCOL_SECONDARY}:
            text = text.casefold()
        if self.strength == UCOL_PRIMARY:
            text = _without_accents(text)
        if self.numeric:
            text = re.sub(
                r"\d+",
                lambda match: f"{int(match.group()):020d}",
                text,
            )
        return text

    def sort_key(self, value: Any) -> bytes:
        """
        Perform the sort key utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.sort key through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = self._comparison_text(value)
        if self.upper_first:
            original = _text(value)
            case_key = "".join("0" if char.isupper() else "1" for char in original)
            text = case_key + "\0" + text
        return text.encode("utf-8", "surrogatepass")

    def strcmp(self, first: Any, second: Any) -> int:
        """
        Perform the strcmp utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.strcmp through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param first: Value supplied for first under the utility contract.
        :param second: Value supplied for second under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        first_key = self.sort_key(first)
        second_key = self.sort_key(second)
        return (first_key > second_key) - (first_key < second_key)

    def find(self, needle: Any, haystack: Any) -> int:
        """
        Perform the find utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.find through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param needle: Value supplied for needle under the utility contract.
        :param haystack: Value supplied for haystack under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._comparison_text(haystack).find(
            self._comparison_text(needle)
        )

    def contains(self, needle: Any, haystack: Any) -> bool:
        """
        Perform the contains utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.contains through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param needle: Value supplied for needle under the utility contract.
        :param haystack: Value supplied for haystack under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.find(needle, haystack) >= 0

    def startswith(self, prefix: Any, text: Any) -> bool:
        """
        Perform the startswith utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.startswith through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param prefix: Text prepended to the formatted or selected result.
        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._comparison_text(text).startswith(
            self._comparison_text(prefix)
        )

    def collation_order(self, value: Any) -> tuple[int, int]:
        """
        Perform the collation order utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.collation order through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = self._comparison_text(value)
        return (ord(text[0]), 1) if text else (0, 0)

    def contractions(self) -> tuple[()]:
        """
        Perform the contractions utility operation under explicit compatibility rules.

        Example:
            Exercise Collator.contractions through a consuming regression::

                python -m pytest -q tests/utils/text/test_text_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ()
