"""
Check LiuXin's language functions against calibre's, now that they have a table to work from.

calibre's language functions read ``iso639.pickle``, which calibre builds and
LiuXin never shipped. Without it:

- ``canonicalize_lang`` returned its input lowercased (``French`` gave ``french``);
- ``get_language`` raised ``FileNotFoundError`` for most codes;
- ``lang_as_iso639_1`` returned three-letter codes;
- ``lang_map`` would have failed on Python 3's dictionaries anyway.

They now read LiuXin's copy of calibre's table, with ISO 639-2's bibliographic and
collective codes added.

Example:
    Run these checks::

        python -m pytest -q tests/utils/test_language_codes.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.metadata.upstream_calibre import _compat as calibre
from LiuXin_alpha.utils.libraries.iso639 import data, find
from LiuXin_alpha.utils.localization import (
    _load_iso639,
    calibre_langcode_to_name,
    canonicalize_lang,
    get_language,
    lang_as_iso639_1,
    lang_map,
    langnames_to_langcodes,
)


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ("fr", "fra"),
        ("FR", "fra"),
        ("fre", "fra"),
        ("fra", "fra"),
        ("French", "fra"),
        ("en-GB", "eng"),
        ("pt_BR", "por"),
        ("zh-Hant", "zho"),
        ("chi", "zho"),
        ("cmn", "cmn"),
        ("Castilian", "spa"),
        ("bnt", "bnt"),
        ("und", "und"),
        ("qqq", None),
        ("xx", None),
        ("", None),
        (None, None),
    ],
)
def test_canonicalize_lang(raw: str | None, code: str | None) -> None:
    """
    Codes, tags and one-word English names give calibre's three-letter code.

    Example:
        >>> test_canonicalize_lang("fr", "fra")


    :param raw: What a book or a source says.
    :param code: The code expected.
    :return: None after the code is checked.
    """
    assert canonicalize_lang(raw) == code


#: ISO 639-2 collective codes that are also the name of one language in calibre's table.
#: calibre, which lacks the codes, reads them as the names; LiuXin reads them as codes.
COLLECTIVE_CODES_THAT_ARE_NAMES = {
    "bih": "ibh",
    "day": "dai",
    "nai": "bio",
    "sio": "xsi",
    "tai": "taw",
}


def test_every_language_calibre_knows_canonicalizes_as_calibre_does() -> None:
    """
    For every code, two-letter code and English name in calibre's table, the answer is calibre's.

    The exceptions are the few ISO 639-2 collective codes that calibre reads as names.

    Example:
        >>> test_every_language_calibre_knows_canonicalizes_as_calibre_does()


    :return: None after every entry is compared.
    """
    table = calibre._load_iso639()
    differ = {}
    for raw in [
        *table["by_3"],
        *table["2to3"],
        *(name for name in table["name_map"] if " " not in name and "-" not in name),
    ]:
        if canonicalize_lang(raw) != calibre.canonicalize_lang(raw):
            differ[raw] = calibre.canonicalize_lang(raw)
            assert canonicalize_lang(raw) == raw
            continue
        assert lang_as_iso639_1(raw) == calibre.lang_as_iso639_1(raw), raw
    assert differ == COLLECTIVE_CODES_THAT_ARE_NAMES


def test_two_letter_and_bibliographic_codes_round_trip() -> None:
    """
    A two-letter code goes to three letters and back; a bibliographic code reaches the same language.

    Example:
        >>> test_two_letter_and_bibliographic_codes_round_trip()


    :return: None after every code is checked.
    """
    table = _load_iso639()
    for two, three in table["2to3"].items():
        assert canonicalize_lang(two) == three
        assert lang_as_iso639_1(three) == two
    for bibliographic, terminologic in table["3bto3t"].items():
        assert canonicalize_lang(bibliographic) == terminologic
        assert lang_as_iso639_1(bibliographic) == lang_as_iso639_1(terminologic)
        assert get_language(bibliographic) == get_language(terminologic)


def test_names_are_calibre_s_and_never_raise() -> None:
    """
    Every known code has calibre's English name; an unknown one is given back, not raised on.

    Example:
        >>> test_names_are_calibre_s_and_never_raise()


    :return: None after the names are checked.
    """
    for code in _load_iso639()["by_3"]:
        assert get_language(code)
    assert [
        get_language(code) for code in ("fr", "fre", "ell", "en_GB", "und", "qqq")
    ] == [
        "French",
        "French",
        "Greek, Modern (1453-)",
        "English (UK)",
        "Unknown",
        "qqq",
    ]
    assert calibre_langcode_to_name("deu") == "German"
    assert calibre_langcode_to_name("qqq") == "qqq"


def test_the_language_list_and_name_lookup_work() -> None:
    """
    The full list of languages builds, and names map back to codes; both once failed on Python 3.

    Example:
        >>> test_the_language_list_and_name_lookup_work()


    :return: None after the list and lookup are checked.
    """
    languages = lang_map()
    assert len(languages) > 7900 and languages["fra"] == "French"
    assert langnames_to_langcodes(["French", "Klingon", "Nonsense"]) == {
        "French": "fra",
        "Klingon": "tlh",
        "Nonsense": None,
    }


def test_the_iso_639_2_list_reads_its_first_language() -> None:
    """
    The shipped list starts with byte-order marks; they no longer hide Afar.

    Example:
        >>> test_the_iso_639_2_list_reads_its_first_language()


    :return: None after the first row is checked.
    """
    assert data[0]["iso639_2_b"] == "aar"
    assert find(iso639_2="aar")["name"] == "Afar"
    assert not [row for row in data if "﻿" in "".join(row.values())]
