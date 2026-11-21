"""
Build calibre's ISO 639 language table from the language data LiuXin ships.

calibre's language functions read a table that calibre generates when it is built
(``iso639.pickle`` in older calibres, ``iso639.calibre_msgpack`` now). LiuXin
ships the same table, built from the iso-codes project with calibre's rules by
``scripts/build_upstream_calibre_iso_codes.py``, as ``iso_codes.json.gz`` beside
this module. It covers ISO 639-3, about 7,900 languages. It does not hold ISO 639-2's
bibliographic codes (``fre``, ``ger``, ``chi``) or its collective codes (``afa``,
``bnt``). Library catalogues use those, so they are added from the ISO 639-2 list
that also ships here.

Example:
    >>> table = build_iso639_table(read_iso_codes()["iso639"], [])
    >>> table["2to3"]["fr"], table["3to2"]["fra"], table["by_3"]["cmn"]
    ('fra', 'fr', 'Chinese, Mandarin')
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

__all__ = ["ISO_CODES", "build_iso639_table", "read_iso_codes"]

#: calibre's language and country maps, as ``scripts/build_upstream_calibre_iso_codes.py`` writes them.
ISO_CODES = Path(__file__).with_name("iso_codes.json.gz")


def read_iso_codes() -> dict[str, Any]:
    """
    Read calibre's language and country maps; calibre's sets are stored as sorted lists.

    Example:
        >>> sorted(read_iso_codes())
        ['iso3166', 'iso639', 'source']


    :return: The maps, keyed ``iso639`` and ``iso3166``, and ``source``, the
        iso-codes release they were built from.
    """
    decoded: dict[str, Any] = json.loads(gzip.decompress(ISO_CODES.read_bytes()))
    return decoded


def _field(entry: Mapping[str, Any], key: str) -> str:
    """
    Read one column of an ISO 639-2 row as lowercase text, empty when it is missing.

    Example:
        >>> _field({"iso639_1": " FR "}, "iso639_1"), _field({}, "iso639_1")
        ('fr', '')


    :param entry: The row.
    :param key: The column.
    :return: The value.
    """
    value = entry.get(key)
    return str(value).strip().lower() if value else ""


def _names(raw: Any) -> list[str]:
    """
    Split a language's English names, which ISO 639-2 separates with semicolons.

    Example:
        >>> _names("Spanish; Castilian"), _names("")
        (['Spanish', 'Castilian'], [])


    :param raw: The names.
    :return: Each name, in order.
    """
    return [name.strip() for name in str(raw or "").split(";") if name.strip()]


def build_iso639_table(
    calibre: Mapping[str, Any], iso639_2: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """
    Combine calibre's ISO 639 maps with what ISO 639-2 adds: bibliographic and collective codes.

    calibre's entries win wherever both have one, so the codes and names match
    calibre's. A bibliographic code maps to its terminologic one (``fre`` to ``fra``),
    which is how calibre stores the language.

    Example:
        >>> calibre = {"by_2": {"fr": "French"}, "by_3": {"fra": "French"}, "2to3": {"fr": "fra"},
        ...            "3to2": {"fra": "fr"}, "name_map": {"french": "fra"}}
        >>> table = build_iso639_table(calibre, [
        ...     {"iso639_2_b": "fre", "iso639_2_t": "fra", "iso639_1": "fr", "name": "French"},
        ...     {"iso639_2_b": "bnt", "iso639_2_t": "", "iso639_1": "", "name": "Bantu languages"},
        ...     {"iso639_2_b": "qaa-qtz", "iso639_2_t": "", "iso639_1": "", "name": "Reserved for local use"},
        ... ])
        >>> table["3bto3t"]["fre"], table["by_3b"]["fre"], table["by_3"]["bnt"], table["3to2"]["fre"]
        ('fra', 'French', 'Bantu languages', 'fr')


    :param calibre: calibre's maps: ``by_2``, ``by_3``, ``2to3``, ``3to2`` and ``name_map``.
    :param iso639_2: The rows of the ISO 639-2 list (``iso639_2_b``, ``iso639_2_t``,
        ``iso639_1``, ``name``).
    :return: calibre's maps, with ``by_3t`` the same as ``by_3``, plus ``by_3b`` and
        ``3bto3t`` for bibliographic codes, and ``codes2`` and ``codes3`` as sets.
    """
    by_2: dict[str, str] = dict(calibre["by_2"])
    by_3: dict[str, str] = dict(calibre["by_3"])
    two_to_three: dict[str, str] = dict(calibre["2to3"])
    three_to_two: dict[str, str] = dict(calibre["3to2"])
    name_map: dict[str, str] = dict(calibre["name_map"])
    by_3b: dict[str, str] = {}
    b_to_t: dict[str, str] = {}
    for entry in iso639_2:
        bibliographic = _field(entry, "iso639_2_b")
        if len(bibliographic) != 3 or not bibliographic.isalpha():
            continue  # "qaa-qtz", a range reserved for local use
        terminologic = _field(entry, "iso639_2_t") or bibliographic
        two = _field(entry, "iso639_1")
        names = _names(entry.get("name"))
        name = by_3.setdefault(terminologic, names[0] if names else terminologic)
        by_3b[bibliographic] = name
        if terminologic != bibliographic:
            b_to_t[bibliographic] = terminologic
        if two:
            by_2.setdefault(two, name)
            two_to_three.setdefault(two, terminologic)
            three_to_two.setdefault(terminologic, two)
        if terminologic in three_to_two:
            three_to_two.setdefault(bibliographic, three_to_two[terminologic])
        for variant in names:
            name_map.setdefault(variant.lower(), terminologic)
    return {
        "by_2": by_2,
        "by_3": by_3,
        "by_3t": by_3,
        "by_3b": by_3b,
        "2to3": two_to_three,
        "3to2": three_to_two,
        "3bto3t": b_to_t,
        "name_map": name_map,
        "codes2": set(by_2),
        "codes3": set(by_3),
    }
