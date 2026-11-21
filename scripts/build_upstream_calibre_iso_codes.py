"""
Build the language and country code maps the ported calibre code reads.

calibre builds ``iso639.calibre_msgpack`` and ``iso3166.calibre_msgpack`` from
the iso-codes project's JSON data in ``setup/translations.py`` (the ``ISO639``
and ``ISO3166`` commands). This script applies the same rules to an iso-codes
``json`` folder, such as ``/usr/share/iso-codes/json`` from the Debian
``iso-codes`` package, and writes both maps to one gzipped JSON file. Sets are
stored as sorted lists.

Example:
    Rebuild the shipped maps from the system's iso-codes::

        python scripts/build_upstream_calibre_iso_codes.py /usr/share/iso-codes/json 4.16.0
"""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

DESTINATION = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "LiuXin_alpha"
    / "utils"
    / "libraries"
    / "iso639"
    / "iso_codes.json.gz"
)


def iso639_maps(entries: list[dict[str, str]]) -> dict[str, object]:
    """
    Build calibre's ISO 639 maps from iso-codes' ``639-3`` entries.

    Example:
        >>> maps = iso639_maps([{"alpha_3": "eng", "alpha_2": "en", "name": "English"}])
        >>> maps["2to3"], maps["name_map"]
        ({'en': 'eng'}, {'english': 'eng'})


    :param entries: The ``639-3`` list from ``iso_639-3.json``.
    :return: ``by_2``, ``by_3``, ``codes2``, ``codes3``, ``2to3``, ``3to2`` and
        ``name_map``, as calibre's ``ISO639`` command builds them.
    """
    by_2: dict[str, str] = {}
    by_3: dict[str, str] = {}
    m2to3: dict[str, str] = {}
    m3to2: dict[str, str] = {}
    name_map: dict[str, str] = {}
    codes2: set[str] = set()
    codes3: set[str] = set()
    for entry in entries:
        two = entry.get("alpha_2")
        three = entry.get("alpha_3")
        if three is None:
            continue
        name = entry.get("inverted_name") or entry.get("name")
        if not name or name[0] in "!~=/'\"":
            continue
        if two is not None:
            by_2[two] = name
            codes2.add(two)
            m2to3[two] = three
            m3to2[three] = two
        codes3.add(three)
        by_3[three] = name
        name_map[name.lower()] = three
    return {
        "by_2": by_2,
        "by_3": by_3,
        "codes2": sorted(codes2),
        "codes3": sorted(codes3),
        "2to3": m2to3,
        "3to2": m3to2,
        "name_map": name_map,
    }


def iso3166_maps(entries: list[dict[str, str]]) -> dict[str, object]:
    """
    Build calibre's ISO 3166 maps from iso-codes' ``3166-1`` entries.

    Example:
        >>> iso3166_maps([{"alpha_2": "GB", "alpha_3": "GBR", "name": "United Kingdom"}])
        {'names': {'GB': 'United Kingdom'}, 'codes': ['GB'], 'three_map': {'GBR': 'GB'}}


    :param entries: The ``3166-1`` list from ``iso_3166-1.json``.
    :return: ``names``, ``codes`` and ``three_map``, as calibre's ``ISO3166``
        command builds them.
    """
    names: dict[str, str] = {}
    codes: set[str] = set()
    three_map: dict[str, str] = {}
    for entry in entries:
        two = entry.get("alpha_2")
        codes.add(two)
        names[two] = entry.get("common_name") or entry.get("name")
        three = entry.get("alpha_3")
        if three:
            three_map[three] = two
    return {"names": names, "codes": sorted(codes), "three_map": three_map}


def main() -> None:
    """
    Read the iso-codes JSON folder and write the gzipped maps.

    Example:
        Run from the command line; see the module docstring.


    :return: None.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("json_folder", type=Path, help="iso-codes' json folder")
    parser.add_argument("version", help="the iso-codes version, recorded in the file")
    args = parser.parse_args()
    languages = json.loads((args.json_folder / "iso_639-3.json").read_text("utf-8"))
    countries = json.loads((args.json_folder / "iso_3166-1.json").read_text("utf-8"))
    document = {
        "source": f"iso-codes {args.version}, built with calibre's setup/translations.py rules",
        "iso639": iso639_maps(languages["639-3"]),
        "iso3166": iso3166_maps(countries["3166-1"]),
    }
    raw = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    # mtime=0 keeps the file byte-identical between builds of the same data.
    DESTINATION.write_bytes(gzip.compress(raw.encode("utf-8"), mtime=0))


if __name__ == "__main__":
    main()
