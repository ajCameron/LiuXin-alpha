"""
Tools to process and understand languages codes.
"""

from copy import deepcopy

from past.builtins import unicode

from typing import Optional

from LiuXin_alpha.utils.libraries.iso639 import find


def canonicalize_lang(lang: str, iso_639_1: bool = False, iso_639_2: bool = False) -> Optional[str]:
    """
    Attempts to bring the language name into a form where it'll be recognized by the find function.

    Returns the name of the function.
    Assumes, by default, that you want the name of the language. Other options are the iso_639_1 code.
    Or the iso_629_2 code.
    Asking for both currently throws an error.
    :param lang: The raw language string to try and normalize
    :param iso_639_1: Is the string an iso_639_1 string? (2 letter code)
    :param iso_639_2: Is the string an iso_639_2 string? (3 letter code)
    :return:
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
    :param lang:
    :return iso639_1:
    """
    lang = deepcopy(lang)
    return canonicalize_lang(lang, iso_639_1=True)
