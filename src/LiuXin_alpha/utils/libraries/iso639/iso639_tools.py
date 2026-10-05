"""
Resolve ISO 639 language codes and names through normalized lookup tables.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise iso639 tools through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""

from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

from typing import Optional

from LiuXin_alpha.utils.libraries.iso639 import find


def canonicalize_lang(lang: str, iso_639_1: bool = False, iso_639_2: bool = False) -> Optional[str]:
    """
    Attempts to bring the language name into a form where it'll be recognized by the find function.

    Example:
        Exercise canonicalize lang through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param lang: Value supplied for lang under the utility contract.
    :param iso_639_1: Value supplied for iso 639 1 under the utility contract.
    :param iso_639_2: Value supplied for iso 639 2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    assert (not iso_639_1) or (not iso_639_2), "No asking for two language codes at the same time."

    lang = deepcopy(lang)
    # If None, returning None
    if not lang:
        return None

    # coercing to unicode, if required
    if not isinstance(lang, unicode):
        lang = lang.decode("utf-8", "ignore")
    lang = lang.lower().strip()

    if not lang:
        return None
    lang = lang.replace("_", "-").partition("-")[0].strip()
    if not lang:
        return None

    try:
        return_candidate = find(lang)
        if return_candidate is not None:
            if iso_639_1:
                return return_candidate["iso639_1"]
            elif iso_639_2:
                return return_candidate["iso639_2_b"]
            else:
                return return_candidate["name"]
    except ValueError:
        return None

    # Todo: Replace this with icu_upper
    try:
        lang_upper = lang[0].upper() + lang[1:]
        return_candidate = find(lang_upper)
        if return_candidate:
            if iso_639_1:
                return return_candidate["iso639_1"]
            elif iso_639_2:
                return return_candidate["iso639_2_b"]
            else:
                return return_candidate["name"]
    except IndexError:
        return None


def lang_as_iso639_1(lang):
    """
    Tries to render the language as an iso639_1 code.

    Example:
        Exercise lang as iso639 1 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param lang: Value supplied for lang under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    lang = deepcopy(lang)
    return canonicalize_lang(lang, iso_639_1=True)
